use dioxus::prelude::*;

#[component]
pub fn Separator() -> Element {
    rsx! {
        hr { class: "df-separator" }
    }
}
