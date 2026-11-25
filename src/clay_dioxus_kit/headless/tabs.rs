//! Tabs (`ct-tabs`): one tab list choosing which of several panels is shown (WAI-ARIA tabs,
//! automatic activation).
//!
//! Parts: `list`, `tab`, `panel`. States: on a `tab` and a `panel`, `data-state` is `active` or
//! `inactive`, with `aria-selected` mirroring it on a tab; a `tab` may carry `data-disabled`.
//! Axis: `data-orientation` on the root. The tab list is data (`items`), not child components,
//! because roving focus needs the order and the disabled ones without measuring or registering
//! anything; the panels are children (`TabPanel`), shown when their `value` is the selected one.
//! The selection is controlled: the caller owns `value` and answers `on_value_change`.
//!
//! Keys, on a focused tab, after the pair of arrows that matches the orientation: next and
//! previous (wrapping, skipping disabled tabs), `Home` and `End` first and last. A key selects
//! as well as focuses (automatic activation), and `Enter`/`Space` on a button already click it.
//!
//! # Renderer limits
//!
//! * Moving focus and the roving `tabindex` (the selected tab `0`, the rest `-1`) need a renderer
//!   whose elements take focus; Blitz panics on `tabindex` (`KIT-052`), so both are gated on
//!   [`Renderer::takes_focus`]. On Blitz the arrow keys still change the selection, and every tab
//!   is a tab stop (they are native buttons).
use super::Orientation;
use super::sections::Renderer;
use super::use_focus_ring;
use super::use_instance_id;
use dioxus::prelude::*;
use std::rc::Rc;

/// One tab of the list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TabItem {
    /// Identifies the tab and its panel; ASCII letters, digits, `-` and `_` only, because it is
    /// part of an element id.
    pub value: String,
    /// The visible, accessible text of the tab.
    pub label: String,
    pub disabled: bool,
}

impl TabItem {
    pub fn new(value: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            label: label.into(),
            disabled: false,
        }
    }

    pub fn disabled(mut self) -> Self {
        self.disabled = true;
        self
    }
}

/// What a panel needs from the `Tabs` it sits in.
#[derive(Clone, Copy)]
struct TabsContext {
    id: usize,
    value: ReadSignal<String>,
}

fn tab_id(instance: usize, value: &str) -> String {
    format!("ct-tabs-{instance}-tab-{value}")
}

fn panel_id(instance: usize, value: &str) -> String {
    format!("ct-tabs-{instance}-panel-{value}")
}

/// The tab a key moves to from the tab at `from`, if the key moves at all. Pure: wraps at the
/// ends and skips disabled tabs. A list with no other enabled tab moves nowhere.
fn key_target(
    items: &[TabItem],
    from: usize,
    orientation: Orientation,
    key: &Key,
) -> Option<usize> {
    let (previous, next) = match orientation {
        Orientation::Horizontal => (NamedKey::ArrowLeft, NamedKey::ArrowRight),
        Orientation::Vertical => (NamedKey::ArrowUp, NamedKey::ArrowDown),
    };
    let enabled = |index: &usize| !items[*index].disabled;
    let count = items.len();
    let target = match key {
        Key::Named(named) if *named == next => {
            (1..count).map(|step| (from + step) % count).find(enabled)
        }
        Key::Named(named) if *named == previous => (1..count)
            .map(|step| (from + count - step) % count)
            .find(enabled),
        Key::Named(NamedKey::Home) => (0..count).find(enabled),
        Key::Named(NamedKey::End) => (0..count).rev().find(enabled),
        _ => None,
    };
    target.filter(|target| *target != from)
}

/// Refuses props that break an invariant (`KIT-023`): an empty list, a value that cannot be part
/// of an element id, two tabs with one value, or a selection that is not a tab.
fn validate(items: &[TabItem], selected: &str) {
    assert!(!items.is_empty(), "Tabs requires at least one tab");
    for (index, item) in items.iter().enumerate() {
        assert!(
            !item.value.is_empty()
                && item
                    .value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
            "Tabs: tab value {:?} must be ASCII letters, digits, `-` and `_`",
            item.value
        );
        assert!(
            items[..index].iter().all(|other| other.value != item.value),
            "Tabs: two tabs share the value {:?}",
            item.value
        );
    }
    assert!(
        items.iter().any(|item| item.value == selected),
        "Tabs: the selected value {selected:?} is not a tab"
    );
}

#[component]
pub fn Tabs(
    items: Vec<TabItem>,
    /// The `value` of the selected tab.
    value: ReadSignal<String>,
    on_value_change: EventHandler<String>,
    /// The accessible name of the tab list.
    label: String,
    #[props(default)] orientation: Orientation,
    #[props(default = Renderer::Blitz)] renderer: Renderer,
    /// The panels.
    children: Element,
) -> Element {
    let selected = value();
    validate(&items, &selected);
    let instance = use_instance_id();
    use_context_provider(|| TabsContext {
        id: instance,
        value,
    });
    let ring = use_focus_ring();
    let takes_focus = renderer.takes_focus();
    let items = Rc::new(items);
    let tabs = items.iter().enumerate().map(|(index, item)| {
        let items = Rc::clone(&items);
        let own = item.value.clone();
        let active = item.value == selected;
        let disabled = item.disabled;
        rsx! {
            button {
                key: "{item.value}",
                r#type: "button",
                role: "tab",
                id: tab_id(instance, &item.value),
                "data-part": "tab",
                "aria-selected": if active { "true" } else { "false" },
                "aria-controls": panel_id(instance, &item.value),
                "data-state": if active { "active" } else { "inactive" },
                "data-disabled": if disabled { Some("") } else { None },
                tabindex: if takes_focus { Some(if active { "0" } else { "-1" }) } else { None },
                disabled,
                onmounted: {
                    let own = own.clone();
                    move |event| {
                        if takes_focus {
                            ring.register(&own, event);
                        }
                    }
                },
                onclick: {
                    let own = own.clone();
                    move |_| {
                        if !disabled && !active {
                            on_value_change.call(own.clone());
                        }
                    }
                },
                onkeydown: move |event| {
                    let Some(target) = key_target(&items, index, orientation, &event.key()) else {
                        return;
                    };
                    event.prevent_default();
                    let target = items[target].value.clone();
                    if takes_focus {
                        ring.move_to(&target);
                    }
                    on_value_change.call(target);
                },
                "{item.label}"
            }
        }
    });
    rsx! {
        div { class: "ct-tabs", "data-orientation": orientation.as_str(),
            div {
                "data-part": "list",
                role: "tablist",
                "aria-label": "{label}",
                "aria-orientation": orientation.as_str(),
                {tabs}
            }
            {children}
        }
    }
}

/// The panel of the tab with the same `value`: its children are mounted only while that tab is
/// selected, and an inactive panel is `hidden`.
#[component]
pub fn TabPanel(value: String, children: Element) -> Element {
    let tabs = use_context::<TabsContext>();
    let active = tabs.value.read().as_str() == value;
    rsx! {
        div {
            role: "tabpanel",
            id: panel_id(tabs.id, &value),
            "data-part": "panel",
            "aria-labelledby": tab_id(tabs.id, &value),
            "data-state": if active { "active" } else { "inactive" },
            hidden: !active,
            if active {
                {children}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;
    use dioxus_storm::Storm;

    fn items() -> Vec<TabItem> {
        vec![
            TabItem::new("overview", "Overview"),
            TabItem::new("activity", "Activity").disabled(),
            TabItem::new("files", "Files"),
            TabItem::new("settings", "Settings"),
        ]
    }

    fn pressed(key: NamedKey, from: usize, orientation: Orientation) -> Option<usize> {
        key_target(&items(), from, orientation, &Key::Named(key))
    }

    #[test]
    fn arrows_wrap_and_skip_disabled_tabs() {
        use Orientation::Horizontal;
        assert_eq!(pressed(NamedKey::ArrowRight, 0, Horizontal), Some(2));
        assert_eq!(pressed(NamedKey::ArrowRight, 3, Horizontal), Some(0));
        assert_eq!(pressed(NamedKey::ArrowLeft, 0, Horizontal), Some(3));
        assert_eq!(pressed(NamedKey::ArrowLeft, 2, Horizontal), Some(0));
        assert_eq!(pressed(NamedKey::Home, 3, Horizontal), Some(0));
        assert_eq!(pressed(NamedKey::End, 0, Horizontal), Some(3));
    }

    #[test]
    fn home_and_end_on_the_end_tab_move_nowhere() {
        assert_eq!(pressed(NamedKey::Home, 0, Orientation::Horizontal), None);
        assert_eq!(pressed(NamedKey::End, 3, Orientation::Horizontal), None);
    }

    #[test]
    fn only_the_arrow_pair_of_the_orientation_moves() {
        assert_eq!(
            pressed(NamedKey::ArrowDown, 0, Orientation::Horizontal),
            None
        );
        assert_eq!(
            pressed(NamedKey::ArrowRight, 0, Orientation::Vertical),
            None
        );
        assert_eq!(
            pressed(NamedKey::ArrowDown, 0, Orientation::Vertical),
            Some(2)
        );
        assert_eq!(
            pressed(NamedKey::ArrowUp, 0, Orientation::Vertical),
            Some(3)
        );
        let other = key_target(
            &items(),
            0,
            Orientation::Horizontal,
            &Key::Character("a".into()),
        );
        assert_eq!(other, None);
    }

    #[test]
    fn a_list_with_one_enabled_tab_moves_nowhere() {
        let only = [TabItem::new("a", "A"), TabItem::new("b", "B").disabled()];
        let key = Key::Named(NamedKey::ArrowRight);
        assert_eq!(key_target(&only, 0, Orientation::Horizontal, &key), None);
    }

    /// The caller of `Tabs`: owns the selection and records what it was asked.
    #[derive(Clone)]
    struct Probe {
        renderer: Renderer,
        orientation: Orientation,
    }

    fn app(probe: Probe) -> Element {
        let mut value = use_signal(|| "overview".to_string());
        rsx! {
            Tabs {
                items: items(),
                value: value(),
                on_value_change: move |next| value.set(next),
                label: "Sections".to_string(),
                orientation: probe.orientation,
                renderer: probe.renderer,
                TabPanel { value: "overview".to_string(), p { "Overview body" } }
                TabPanel { value: "activity".to_string(), p { "Activity body" } }
                TabPanel { value: "files".to_string(), p { "Files body" } }
                TabPanel { value: "settings".to_string(), p { "Settings body" } }
            }
        }
    }

    fn render(renderer: Renderer) -> Page {
        Page::with_props(
            app,
            Probe {
                renderer,
                orientation: Orientation::Horizontal,
            },
        )
    }

    #[test]
    fn the_tab_list_and_panels_are_wired_for_assistive_technology() {
        let page = render(Renderer::Browser);
        let list = page.by_role("tablist", "Sections");
        assert_eq!(
            page.attr(list, "aria-orientation").as_deref(),
            Some("horizontal")
        );
        assert_eq!(page.attr(list, "data-part").as_deref(), Some("list"));
        let root = page.by_class("ct-tabs");
        assert_eq!(
            page.parts(root),
            ["list", "panel", "panel", "panel", "panel"]
        );
        let overview = page.by_role("tab", "Overview");
        let panel = page.by_role("tabpanel", "Overview body");
        assert_eq!(
            page.attr(overview, "aria-selected").as_deref(),
            Some("true")
        );
        assert_eq!(page.attr(overview, "data-state").as_deref(), Some("active"));
        assert_eq!(page.attr(panel, "data-state").as_deref(), Some("active"));
        // The two reference each other by id.
        assert_eq!(page.attr(overview, "aria-controls"), page.attr(panel, "id"));
        assert_eq!(
            page.attr(panel, "aria-labelledby"),
            page.attr(overview, "id")
        );
        let files = page.by_role("tab", "Files");
        assert_eq!(page.attr(files, "aria-selected").as_deref(), Some("false"));
        assert_eq!(page.attr(files, "data-state").as_deref(), Some("inactive"));
    }

    #[test]
    fn only_the_selected_panel_has_content_and_the_others_are_hidden() {
        let page = render(Renderer::Browser);
        assert!(page.has_role("tabpanel", "Overview body"));
        // The inactive panels are `hidden`, so they are not accessible at all.
        assert_eq!(page.outline().matches("tabpanel").count(), 1);
        assert!(!page.text(page.by_class("ct-tabs")).contains("Files body"));
    }

    #[test]
    fn roving_tabindex_is_set_only_where_the_renderer_takes_focus() {
        let page = render(Renderer::Browser);
        assert_eq!(
            page.attr(page.by_role("tab", "Overview"), "tabindex")
                .as_deref(),
            Some("0")
        );
        assert_eq!(
            page.attr(page.by_role("tab", "Files"), "tabindex")
                .as_deref(),
            Some("-1")
        );
        let page = render(Renderer::Blitz);
        assert_eq!(page.attr(page.by_role("tab", "Overview"), "tabindex"), None);
        assert_eq!(page.attr(page.by_role("tab", "Files"), "tabindex"), None);
    }

    #[test]
    fn a_click_selects_that_tab_and_shows_its_panel() {
        let mut page = render(Renderer::Blitz);
        let settings = page.by_role("tab", "Settings");
        page.click(settings);
        let settings = page.by_role("tab", "Settings");
        assert_eq!(
            page.attr(settings, "aria-selected").as_deref(),
            Some("true")
        );
        assert!(page.has_role("tabpanel", "Settings body"));
        assert!(!page.has_role("tabpanel", "Overview body"));
    }

    #[test]
    fn a_disabled_tab_ignores_its_click() {
        let mut page = render(Renderer::Blitz);
        let activity = page.by_role("tab", "Activity");
        assert_eq!(page.attr(activity, "data-disabled").as_deref(), Some(""));
        assert_eq!(page.attr(activity, "disabled").as_deref(), Some("true"));
        page.click(activity);
        assert!(page.has_role("tabpanel", "Overview body"));
    }

    #[test]
    fn arrow_keys_select_the_next_enabled_tab_and_move_focus_to_it() {
        let mut page = render(Renderer::Browser);
        let overview = page.by_role("tab", "Overview");
        let files = page.by_role("tab", "Files");
        // Each tab hands the widget its element when it mounts; the recorder notes focus.
        let _on_overview = page.mount(overview);
        let on_files = page.mount(files);
        assert_eq!(on_files.get(), None);
        page.key(overview, Key::Named(NamedKey::ArrowRight));
        // Activity is disabled, so the selection lands on Files and focus follows it.
        assert_eq!(on_files.get(), Some(true));
        let files = page.by_role("tab", "Files");
        assert_eq!(page.attr(files, "aria-selected").as_deref(), Some("true"));
        assert_eq!(page.attr(files, "tabindex").as_deref(), Some("0"));
        assert_eq!(
            page.attr(page.by_role("tab", "Overview"), "tabindex")
                .as_deref(),
            Some("-1")
        );
        assert!(page.has_role("tabpanel", "Files body"));
    }

    #[test]
    fn on_blitz_the_keys_select_without_touching_focus() {
        let mut page = render(Renderer::Blitz);
        let overview = page.by_role("tab", "Overview");
        page.key(overview, Key::Named(NamedKey::End));
        assert!(page.has_role("tabpanel", "Settings body"));
    }

    #[test]
    fn a_vertical_list_answers_the_vertical_arrows() {
        let mut page = Page::with_props(
            app,
            Probe {
                renderer: Renderer::Blitz,
                orientation: Orientation::Vertical,
            },
        );
        let root = page.by_class("ct-tabs");
        assert_eq!(
            page.attr(root, "data-orientation").as_deref(),
            Some("vertical")
        );
        let overview = page.by_role("tab", "Overview");
        page.key(overview, Key::Named(NamedKey::ArrowRight));
        assert!(
            page.has_role("tabpanel", "Overview body"),
            "a horizontal arrow is ignored"
        );
        page.key(overview, Key::Named(NamedKey::ArrowDown));
        assert!(page.has_role("tabpanel", "Files body"));
    }

    #[test]
    #[should_panic(expected = "is not a tab")]
    fn a_selected_value_that_is_not_a_tab_is_a_programming_error() {
        validate(&items(), "nope");
    }

    #[test]
    #[should_panic(expected = "share the value")]
    fn two_tabs_with_one_value_are_refused() {
        validate(&[TabItem::new("a", "A"), TabItem::new("a", "Again")], "a");
    }

    #[test]
    #[should_panic(expected = "ASCII letters")]
    fn a_value_that_cannot_be_part_of_an_id_is_refused() {
        validate(&[TabItem::new("two words", "A")], "two words");
    }

    #[test]
    #[should_panic(expected = "at least one tab")]
    fn no_tabs_are_refused() {
        validate(&[], "a");
    }

    /// The storm cannot deliver `mounted` (renderer only), which is what a focusing renderer's
    /// roving focus waits for, so it runs the Blitz arrangement; the focus path has its own test.
    fn owner() -> Element {
        app(Probe {
            renderer: Renderer::Blitz,
            orientation: Orientation::Horizontal,
        })
    }

    #[test]
    fn tabs_survive_any_order_of_events() {
        Storm::of(owner).run_seeds(0..32);
    }
}
