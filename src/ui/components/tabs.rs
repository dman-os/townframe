use dioxus::prelude::*;

#[derive(Clone, Copy)]
struct TabsState {
    selected: Signal<String>,
}

#[component]
pub fn Tabs(default_value: String, children: Element) -> Element {
    use_context_provider(|| TabsState {
        selected: Signal::new(default_value),
    });

    rsx! {
        div { class: "ct-tabs", {children} }
    }
}

#[component]
pub fn TabList(children: Element) -> Element {
    rsx! {
        div {
            class: "ct-tabs-list",
            role: "tablist",
            {children}
        }
    }
}

#[component]
pub fn TabTrigger(value: String, children: Element) -> Element {
    let mut tabs = use_context::<TabsState>();
    let active = tabs.selected.read().as_str() == value;
    let value_for_handler = value.clone();

    rsx! {
        button {
            class: if active { "ct-tab-trigger ct-tab-trigger-active" } else { "ct-tab-trigger" },
            r#type: "button",
            role: "tab",
            aria_selected: active,
            onclick: move |_| tabs.selected.set(value_for_handler.clone()),
            {children}
        }
    }
}

#[component]
pub fn TabContent(value: String, children: Element) -> Element {
    let tabs = use_context::<TabsState>();
    let active = tabs.selected.read().as_str() == value;

    rsx! {
        div {
            class: if active { "ct-tab-content ct-tab-content-active" } else { "ct-tab-content" },
            role: "tabpanel",
            if active { {children} }
        }
    }
}
