use dioxus::prelude::*;
use dioxus_native::winit::event::WindowEvent;
use dioxus_native::winit::window::Window;
use std::sync::Arc;

const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

use ui::arc_index::ArcIndex;
use ui::canvas_probe::CanvasProbe;
use ui::tiles::TilesDemo;

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    // The window knows its own size from the first frame, whereas a Blitz node
    // has no layout to measure until after a pass (mount reports 0x0). So the
    // shell measures and the index just lays out in the numbers it is given.
    let window = dioxus_native::use_window();
    let mut viewport = use_signal(|| logical_size(&window));
    dioxus_native::use_window_event(move |event, _| match event {
        WindowEvent::SurfaceResized(_) | WindowEvent::ScaleFactorChanged { .. } => {
            viewport.set(logical_size(&window));
        }
        _ => {}
    });

    // Tiles prototype experiment (FDR 005).
    // The template router/views (docs, blog) have been superseded by the
    // tiles experiment surface for now; see docs/fdrs/005-tiles-primitive.md.
    rsx! {
        document::Link { rel: "stylesheet", href: TAILWIND_CSS }
        div {
            style: "position:relative; min-height:100vh;",
            TilesDemo { viewport: viewport() }
            // Arc index experiment overlaid on the tiles surface.
            ArcIndex { viewport: viewport() }
            // Infinite-canvas probe (throwaway). Opt-in, so the tile and index
            // surfaces stay usable: run with DAYFRAME_PROBE=1.
            if probe_enabled() {
                CanvasProbe { viewport: viewport() }
            }
        }
    }
}

/// Whether to mount the infinite-canvas probe over the app.
fn probe_enabled() -> bool {
    std::env::var("DAYFRAME_PROBE").is_ok()
}

/// The window's inner size in logical px, i.e. CSS units.
fn logical_size(window: &Arc<dyn Window>) -> (f64, f64) {
    let scale = window.scale_factor();
    let size = window.surface_size();
    (size.width as f64 / scale, size.height as f64 / scale)
}
