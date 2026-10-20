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
        div { class: "df-tabs", {children} }
    }
}

#[component]
pub fn TabList(children: Element) -> Element {
    rsx! {
        div {
            class: "df-tabs-list",
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
            class: if active { "df-tab-trigger df-tab-trigger-active" } else { "df-tab-trigger" },
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
            class: if active { "df-tab-content df-tab-content-active" } else { "df-tab-content" },
            role: "tabpanel",
            if active { {children} }
        }
    }
}
