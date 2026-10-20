use dioxus::prelude::*;

#[component]
pub fn Card(children: Element) -> Element {
    rsx! {
        div {
            class: "df-card",
            "data-slot": "card",
            {children}
        }
    }
}

#[component]
pub fn CardHeader(children: Element) -> Element {
    rsx! {
        div {
            class: "df-card-header",
            "data-slot": "card-header",
            {children}
        }
    }
}

#[component]
pub fn CardContent(children: Element) -> Element {
    rsx! {
        div {
            class: "df-card-content",
            "data-slot": "card-content",
            {children}
        }
    }
}
