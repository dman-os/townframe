use dioxus::prelude::*;
use ui::{ThemeMode, ThemeRoot};

fn main() {
    dioxus::launch(App);
}

#[cfg(target_os = "android")]
#[no_mangle]
fn android_main(app: dioxus_native::AndroidApp) {
    dioxus_native::set_android_app(app);
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
