use daytxt_core::Renderer;
use dioxus::prelude::*;

mod composition;

#[cfg(test)]
mod e2e;

/// Wry unless `DAYTXT_RENDERER=blitz` picks the Blitz launcher, which the `blitz` feature
/// compiles in next to it. Both launchers live in one binary so the feature stays additive.
fn main() {
    match std::env::var("DAYTXT_RENDERER").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("wry") => wry::launch(),
        #[cfg(feature = "blitz")]
        Ok("blitz") => blitz::launch(),
        other => panic!(
            "DAYTXT_RENDERER must be `wry` or (with the `blitz` feature) `blitz`, got {other:?}"
        ),
    }
}

#[component]
fn App() -> Element {
    rsx! {
        daytxt_native::Shell {
            load: daytxt_native::runtime_loader(|| async {
                let store = composition::open_store().await?;
                composition::hosted_runtime(std::sync::Arc::new(store)).await
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
        // A desktop window has no system chrome over its surface.
        Renderer::Browser => {
            rsx! { daytxt_native::WryApplication { runtime, insets: None, viewport: None } }
        }
        #[cfg(feature = "blitz")]
        Renderer::Blitz => rsx! { daytxt_native::BlitzApplication { runtime, insets: None } },
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
                    .with_window(dioxus::desktop::WindowBuilder::new().with_title("daytxt"))
                    // The shell draws its own chrome; Wry's default menu bar adds nothing to it.
                    .with_menu(None),
            )
            .launch(App);
    }
}

#[cfg(feature = "blitz")]
mod blitz {
    use super::*;

    // `LaunchBuilder::new` warns "no renderer" when only `dioxus/native` is on, so Blitz launches
    // through its own entry point.
    pub(crate) fn launch() {
        let config = dioxus_native::Config::new().with_window_attributes(
            dioxus_native::winit::window::WindowAttributes::default().with_title("daytxt"),
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
