use dioxus::prelude::*;
use dioxus_native::winit::event::WindowEvent;
use dioxus_native::winit::window::Window;
use std::sync::Arc;
use ui::{sample_column_specs, ColumnHeight, ColumnRack, ThemeMode, ThemeRoot};

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    let window = dioxus_native::use_window();
    let mut viewport = use_signal(|| logical_size(&window));
    dioxus_native::use_window_event(move |event, _| match event {
        WindowEvent::SurfaceResized(_) | WindowEvent::ScaleFactorChanged { .. } => {
            viewport.set(logical_size(&window));
        }
        _ => {}
    });

    rsx! {
        ThemeRoot {
            family: "material".to_string(),
            mode: ThemeMode::Dark,
            main {
                class: "min-h-screen box-border bg-background p-6 font-sans text-foreground",
                ColumnRack {
                    columns: sample_column_specs(),
                    height: ColumnHeight::Natural,
                    available_width: viewport().0 - 48.0,
                }
            }
        }
    }
}

/// The window's inner size in logical px, i.e. CSS units.
fn logical_size(window: &Arc<dyn Window>) -> (f64, f64) {
    let scale = window.scale_factor();
    let size = window.surface_size();
    (size.width as f64 / scale, size.height as f64 / scale)
}
