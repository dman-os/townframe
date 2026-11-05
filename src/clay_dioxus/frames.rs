//! The browser's animation-frame clock, for coalescing input that is a snapshot of state (a
//! scroll position): only the last one per frame matters, and delivering each to the guest costs
//! a synchronous guest render. Other hosts have no frame source here, so [`coalescing`] is false
//! there and events are delivered as they arrive.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod imp {
    /// Whether events may be held until the next frame. A page can turn it off
    /// (`window.__clayNoCoalesce = true`) to measure the uncoalesced path.
    pub fn coalescing() -> bool {
        let window = web_sys::window().expect("a window");
        !js_sys::Reflect::get(&window, &"__clayNoCoalesce".into())
            .expect("window is readable")
            .is_truthy()
    }

    /// Run `run` at the next animation frame.
    pub fn next_frame(run: impl FnOnce() + 'static) {
        let callback = wasm_bindgen::closure::Closure::once_into_js(run);
        web_sys::window()
            .expect("a window")
            .request_animation_frame(wasm_bindgen::JsCast::unchecked_ref(&callback))
            .expect("the window schedules a frame");
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod imp {
    pub fn coalescing() -> bool {
        false
    }

    pub fn next_frame(_run: impl FnOnce() + 'static) {
        unreachable!("only a coalescing host schedules frames")
    }
}

pub(crate) use imp::coalescing;
pub(crate) use imp::next_frame;
