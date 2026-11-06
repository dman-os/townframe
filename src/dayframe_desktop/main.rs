use dioxus::prelude::*;

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
    rsx! {
        dayframe_native::Shell {
            load: dayframe_native::runtime_loader(|| async {
                let store = composition::open_store().await?;
                composition::desktop_runtime(std::sync::Arc::new(store))
                    .await
                    .map_err(|error| format!("cannot load the store: {error}"))
            }),
        }
    }
}
