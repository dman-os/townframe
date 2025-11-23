#[allow(unused_imports)]
mod interlude {
    #[cfg(feature = "ssr")]
    pub use crate::server::SharedServerCtx;
    #[cfg(feature = "ssr")]
    pub use axum::http::{self, uri::Uri};
    pub use leptos::prelude::*;
    pub use utils_rs::prelude::*;
}
pub mod app;
mod auth;
#[cfg(feature = "ssr")]
pub mod server;

#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    use crate::app::*;
    console_error_panic_hook::set_once();
    leptos::mount::hydrate_body(App);
}
