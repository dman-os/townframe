use dioxus::prelude::*;
use ui::{ThemeMode, ThemeRoot};

mod composition;

#[cfg(test)]
mod e2e;

fn main() {
    dioxus::LaunchBuilder::new()
        .with_cfg(dioxus_native::Config::new().with_window_attributes(
            dioxus_native::winit::window::WindowAttributes::default().with_title("Dayframe"),
        ))
        .launch(App);
}

#[component]
fn App() -> Element {
    let runtime = use_signal(composition::DesktopRuntime::new);
    rsx! {
        ThemeRoot {
            family: "material".to_string(),
            mode: ThemeMode::Dark,
            main {
                class: "min-h-screen box-border bg-background p-6 font-sans text-foreground",
                composition::Application { runtime }
            }
        }
    }
}
