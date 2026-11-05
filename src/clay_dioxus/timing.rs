//! Spike instrumentation (feature `spike-timing`): appends `[kind, start, end, a, b]` rows to
//! `window.__clayTiming`, with `performance.now()` timestamps, for the Playwright spec to read.
//! Without the feature (and off the browser) every call compiles to nothing.

#[cfg(all(
    feature = "spike-timing",
    target_arch = "wasm32",
    target_os = "unknown"
))]
mod imp {
    pub fn now() -> f64 {
        web_sys::window()
            .expect("a window")
            .performance()
            .expect("performance")
            .now()
    }

    pub fn record(kind: &str, start: f64, a: f64, b: f64) {
        let end = now();
        let window = web_sys::window().expect("a window");
        let key = js_sys::JsString::from("__clayTiming");
        let log = match js_sys::Reflect::get(&window, &key) {
            // `Array::from(&JsValue)` would copy the page's array; reinterpret it instead.
            Ok(value) if value.is_object() => {
                wasm_bindgen::JsCast::unchecked_into::<js_sys::Array>(value)
            }
            _ => {
                let log = js_sys::Array::new();
                js_sys::Reflect::set(&window, &key, &log).expect("window is extensible");
                log
            }
        };
        let row = js_sys::Array::new_with_length(5);
        row.set(0, kind.into());
        row.set(1, start.into());
        row.set(2, end.into());
        row.set(3, a.into());
        row.set(4, b.into());
        log.push(&row);
    }
}

#[cfg(not(all(
    feature = "spike-timing",
    target_arch = "wasm32",
    target_os = "unknown"
)))]
mod imp {
    pub fn now() -> f64 {
        0.0
    }
    pub fn record(_kind: &str, _start: f64, _a: f64, _b: f64) {}
}

/// Whether records are kept; callers gate any allocation they only do for a record on this.
pub(crate) const ENABLED: bool = cfg!(all(
    feature = "spike-timing",
    target_arch = "wasm32",
    target_os = "unknown"
));

pub(crate) use imp::now;
pub(crate) use imp::record;
