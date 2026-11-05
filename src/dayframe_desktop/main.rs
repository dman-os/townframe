use dayframe_composition::HostInputs;
use dayframe_composition::Insets;
use dioxus::prelude::*;
use dioxus_native::winit::event::ButtonSource;
use dioxus_native::winit::event::ElementState;
use dioxus_native::winit::event::MouseButton;
use dioxus_native::winit::event::WindowEvent;
use dioxus_native::winit::keyboard::Key;
use dioxus_native::winit::keyboard::NamedKey;
use dioxus_native::winit::window::Window;
use std::sync::Arc;
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
                class: "bg-background font-sans text-foreground",
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
    let host = use_host_inputs();
    rsx! { composition::Application { runtime, host } }
}

/// What the window tells the shell: its logical size (the shell never measures the DOM, and
/// Blitz panics if it tries to from a handler), and the platform back gesture. Android's
/// `KEYCODE_BACK` arrives from winit as `BrowserBack`; a mouse's back button is the desktop
/// equivalent. Neither is a key Blitz delivers to a focused element, so they are taken from the
/// window event stream.
fn use_host_inputs() -> HostInputs {
    let window: Arc<dyn Window> = use_hook(consume_context);
    let logical_size = |window: &Arc<dyn Window>| {
        let size = window.surface_size();
        let scale = window.scale_factor();
        (size.width as f64 / scale, size.height as f64 / scale)
    };
    let initial = logical_size(&window);
    let mut width = use_signal(|| initial.0);
    let mut height = use_signal(|| initial.1);
    let mut platform_back = use_signal(|| 0_u64);
    let can_back = use_signal(|| false);
    // A desktop window has no system chrome over its surface.
    let insets = use_signal(Insets::default);
    dioxus_native::use_window_event(move |event, _| match event {
        WindowEvent::SurfaceResized(_) | WindowEvent::ScaleFactorChanged { .. } => {
            let (w, h) = logical_size(&window);
            width.set(w);
            height.set(h);
        }
        WindowEvent::KeyboardInput { event, .. }
            if event.state == ElementState::Pressed
                && !event.repeat
                && event.logical_key == Key::Named(NamedKey::BrowserBack) =>
        {
            platform_back += 1;
        }
        WindowEvent::PointerButton {
            button: ButtonSource::Mouse(MouseButton::Back),
            state: ElementState::Pressed,
            ..
        } => platform_back += 1,
        _ => {}
    });
    HostInputs {
        width: width.into(),
        height: height.into(),
        insets: insets.into(),
        platform_back,
        can_back,
        // Blitz cannot take keyboard focus (`tabindex` panics it).
        keyboard: false,
        // The development seed: see `HostInputs::start`.
        start: std::env::var("DAYFRAME_SHELL_OPEN").ok(),
    }
}
