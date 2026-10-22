use dioxus::prelude::*;
use ui::ThemeMode;
use ui::ThemeRoot;

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    rsx! {
        ThemeRoot {
            family: "material".to_string(),
            mode: ThemeMode::Dark,
            main {
                class: "min-h-screen box-border bg-background p-6 font-sans text-foreground",
                h1 { "Dayframe" }
            }
        }
    }
}
