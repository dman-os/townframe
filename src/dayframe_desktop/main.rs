use dayframe_composition::HostInputs;
use dayframe_composition::Insets;
use dioxus::prelude::*;
use ui::ThemeMode;
use ui::ThemeRoot;

mod composition;

#[cfg(test)]
mod e2e;

#[cfg(all(feature = "desktop", feature = "native"))]
compile_error!("pick one renderer: `desktop` (Wry) or `native` (Blitz)");

// `LaunchBuilder::new` picks Blitz whenever any crate in the build turns on `dioxus/native`
// (feature unification), so Wry is chosen by name.
#[cfg(feature = "desktop")]
fn main() {
    dioxus::LaunchBuilder::desktop()
        .with_cfg(
            dioxus::desktop::Config::new()
                .with_window(dioxus::desktop::WindowBuilder::new().with_title("Dayframe"))
                // The shell draws its own chrome; Wry's default menu bar adds nothing to it.
                .with_menu(None),
        )
        .launch(App);
}

// `LaunchBuilder::new` warns "no renderer" when only `dioxus/native` is on, so Blitz launches
// through its own entry point.
#[cfg(feature = "native")]
fn main() {
    let config = dioxus_native::Config::new().with_window_attributes(
        dioxus_native::winit::window::WindowAttributes::default().with_title("Dayframe"),
    );
    dioxus_native::launch_cfg(App, Vec::new(), vec![Box::new(config)]);
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

/// What the window tells the shell under Wry: its logical size, read from tao's resize events
/// (the shell never measures the DOM). The webview takes keyboard focus, so the shell's keyboard
/// paths are on. No platform back gesture reaches a desktop webview; the shell's own back button
/// is the only way back.
#[cfg(feature = "desktop")]
fn use_host_inputs() -> HostInputs {
    use dioxus::desktop::tao::event::Event;
    use dioxus::desktop::WindowEvent;
    let window = dioxus::desktop::use_window();
    let logical_size = move || {
        let window = &dioxus::desktop::window().window;
        let size = window.inner_size().to_logical::<f64>(window.scale_factor());
        (size.width, size.height)
    };
    let initial = logical_size();
    let mut width = use_signal(|| initial.0);
    let mut height = use_signal(|| initial.1);
    let platform_back = use_signal(|| 0_u64);
    let can_back = use_signal(|| false);
    // A desktop window has no system chrome over its surface.
    let insets = use_signal(Insets::default);
    dioxus::desktop::use_wry_event_handler(move |event, _| {
        let Event::WindowEvent {
            window_id,
            event: WindowEvent::Resized(_) | WindowEvent::ScaleFactorChanged { .. },
            ..
        } = event
        else {
            return;
        };
        if *window_id == window.window.id() {
            let (w, h) = logical_size();
            width.set(w);
            height.set(h);
        }
    });
    HostInputs {
        width: width.into(),
        height: height.into(),
        insets: insets.into(),
        platform_back,
        can_back,
        keyboard: true,
        // The development seed: see `HostInputs::start`.
        start: std::env::var("DAYFRAME_SHELL_OPEN").ok(),
    }
}

/// What the window tells the shell under Blitz: its logical size (the shell never measures the DOM, and
/// Blitz panics if it tries to from a handler), and the platform back gesture. Android's
/// `KEYCODE_BACK` arrives from winit as `BrowserBack`; a mouse's back button is the desktop
/// equivalent. Neither is a key Blitz delivers to a focused element, so they are taken from the
/// window event stream.
#[cfg(feature = "native")]
fn use_host_inputs() -> HostInputs {
    use dioxus_native::winit::event::ButtonSource;
    use dioxus_native::winit::event::ElementState;
    use dioxus_native::winit::event::MouseButton;
    use dioxus_native::winit::event::WindowEvent;
    use dioxus_native::winit::keyboard::Key;
    use dioxus_native::winit::keyboard::NamedKey;
    use dioxus_native::winit::window::Window;
    use std::sync::Arc;
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
