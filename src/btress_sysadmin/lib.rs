mod interlude {
    pub use leptos::prelude::*;
    pub use utils_rs::prelude::*;
}

mod app;
#[cfg(feature = "ssr")]
mod server;

#[cfg(feature = "ssr")]
mod wit {
    wit_bindgen::generate!({
        generate_all,
        // async: true,
        with: {
            "wasi:http/types@0.2.6": wasip2::http::types,
            "wasi:http/incoming-handler@0.2.6": wasip2::http::incoming_handler,

            "wasi:io/streams@0.2.6": wasip2::io::streams,
            "wasi:io/error@0.2.6": wasip2::io::error,
            "wasi:io/poll@0.2.6": wasip2::io::poll,
            // "wasi:http/outgoing-handler@0.2.6": wasip2::http::outgoing_handler,
        }
    });
}

#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    use crate::app::*;
    console_error_panic_hook::set_once();
    leptos::mount::hydrate_body(App);
}

#[cfg(feature = "ssr")]
use crate::interlude::*;
#[cfg(feature = "ssr")]
use wasip2::http::types::{IncomingRequest, ResponseOutparam};

#[cfg(feature = "ssr")]
struct Component;

#[cfg(feature = "ssr")]
wit::export!(Component with_types_in wit);

#[cfg(feature = "ssr")]
impl wit::exports::wasi::http::incoming_handler::Guest for Component {
    // #[expect(async_fn_in_trait)]
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        any_spawner::Executor::init_tokio().expect(ERROR_TOKIO);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect(ERROR_TOKIO);
        let local = tokio::task::LocalSet::new();
        local
            .block_on(&rt, server::server_main(request, response_out))
            .expect("error handling request");
    }
}
