use dayframe_composition::HostInputs;
use dayframe_composition::Insets;
use dayframe_composition::Renderer;
use dioxus::prelude::*;

mod composition;

#[cfg(test)]
mod e2e;

/// Wry unless `DAYFRAME_RENDERER=blitz` picks the Blitz launcher, which the `blitz` feature
/// compiles in next to it. Both launchers live in one binary so the feature stays additive.
fn main() {
    match std::env::var("DAYFRAME_RENDERER").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("wry") => wry::launch(),
        #[cfg(feature = "blitz")]
        Ok("blitz") => blitz::launch(),
        other => panic!(
            "DAYFRAME_RENDERER must be `wry` or (with the `blitz` feature) `blitz`, got {other:?}"
        ),
    }
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
            ready: Callback::new(|runtime| rsx! { ReadyApp { runtime } }),
        }
    }
}

/// Mounts the shell once the runtime exists. The launcher provides the renderer it started;
/// it fixes which window events exist, so Wry reads its own and Blitz uses the shared native
/// host inputs.
#[component]
fn ReadyApp(runtime: Signal<Option<composition::CompositionRuntime>>) -> Element {
    match use_context::<Renderer>() {
        Renderer::Browser => {
            let initial = runtime
                .read()
                .clone()
                .expect("ReadyApp mounts only with a built runtime");
            let runtime = use_signal(|| initial);
            let host = wry::use_host_inputs();
            rsx! { dayframe_composition::Application { runtime, host } }
        }
        #[cfg(feature = "blitz")]
        Renderer::Blitz => rsx! { dayframe_native::BlitzApplication { runtime, insets: None } },
        #[cfg(not(feature = "blitz"))]
        Renderer::Blitz => unreachable!("only the `blitz` feature launches Blitz"),
    }
}

mod wry {
    use super::*;

    // `LaunchBuilder::new` picks Blitz whenever any crate in the build turns on `dioxus/native`
    // (feature unification), so Wry is chosen by name.
    pub(crate) fn launch() {
        dioxus::LaunchBuilder::desktop()
            .with_context(Renderer::Browser)
            .with_cfg(
                dioxus::desktop::Config::new()
                    .with_window(dioxus::desktop::WindowBuilder::new().with_title("Dayframe"))
                    // The shell draws its own chrome; Wry's default menu bar adds nothing to it.
                    .with_menu(None),
            )
            .launch(App);
    }

    /// What the window tells the shell under Wry: its logical size, read from tao's resize events
    /// (the shell never measures the DOM). The webview is a browser engine, so the shell's keyboard
    /// paths are on and a split drag survives the pointer leaving the window. No platform back gesture reaches a desktop webview; the shell's own back button
    /// is the only way back.
    pub(crate) fn use_host_inputs() -> HostInputs {
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
            renderer: Renderer::Browser,
            // The development seed: see `HostInputs::start`.
            start: std::env::var("DAYFRAME_SHELL_OPEN").ok(),
        }
    }
}

#[cfg(feature = "blitz")]
mod blitz {
    use super::*;

    // `LaunchBuilder::new` warns "no renderer" when only `dioxus/native` is on, so Blitz launches
    // through its own entry point.
    pub(crate) fn launch() {
        let config = dioxus_native::Config::new().with_window_attributes(
            dioxus_native::winit::window::WindowAttributes::default().with_title("Dayframe"),
        );
        dioxus_native::launch_cfg(
            App,
            vec![Box::new(|| {
                Box::new(Renderer::Blitz) as Box<dyn std::any::Any>
            })],
            vec![Box::new(config)],
        );
    }
}
