use dioxus::prelude::*;
use ui::ThemeMode;
use ui::ThemeRoot;

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
    let mut runtime = use_signal(|| None::<composition::CompositionRuntime>);
    let mut failure = use_signal(|| None::<String>);
    use_future(move || async move {
        let built = async {
            let store = composition::open_store().await?;
            composition::desktop_runtime(std::sync::Arc::new(store))
                .await
                .map_err(|error| format!("cannot load the store: {error}"))
        }
        .await;
        match built {
            Ok(built) => runtime.set(Some(built)),
            Err(error) => failure.set(Some(error)),
        }
    });
    rsx! {
        ThemeRoot {
            family: "material".to_string(),
            mode: ThemeMode::Dark,
            main {
                class: "min-h-screen box-border bg-background p-6 font-sans text-foreground",
                if let Some(error) = failure() {
                    p { role: "alert", "Dayframe cannot start: {error}" }
                } else if runtime.read().is_some() {
                    ReadyApp { runtime }
                } else {
                    p { role: "status", "Loading Dayframe…" }
                }
            }
        }
    }
}

/// Mounts the shell once the runtime exists; `Application` takes the runtime as a signal.
#[component]
fn ReadyApp(runtime: Signal<Option<composition::CompositionRuntime>>) -> Element {
    let initial = runtime
        .read()
        .clone()
        .expect("ReadyApp mounts only with a built runtime");
    let runtime = use_signal(|| initial);
    rsx! { composition::Application { runtime } }
}
