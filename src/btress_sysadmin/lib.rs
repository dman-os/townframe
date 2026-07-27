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
    // #[allow(async_fn_in_trait)]
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        std::env::set_var("RUST_BACKTRACE", "full");
        // FIXME: use wit_bindgen (async-spawner) instead
        // let rt = tokio::runtime::Builder::new_current_thread()
        //     .enable_all()
        //     .build().expect(ERROR_TOKIO);
        // any_spawner::Executor::init_tokio().expect(ERROR_TOKIO);
        struct WitExecutor;
        impl any_spawner::CustomExecutor for WitExecutor {
            /// Spawns a future, usually on a thread pool.
            fn spawn(&self, fut: any_spawner::PinnedFuture<()>) {
                wit_bindgen::spawn_local(fut);
            }
            /// Spawns a local future. May require calling `poll_local` to make progress.
            fn spawn_local(&self, fut: any_spawner::PinnedLocalFuture<()>) {
                wit_bindgen::spawn_local(fut);
            }
            /// Polls the executor, if it supports polling. Implementations should ideally be
            /// non-blocking or use mechanisms like `try_tick` or `try_borrow_mut` to handle
            /// re-entrant calls safely.
            fn poll_local(&self) {}
        }
        any_spawner::Executor::init_custom_executor(WitExecutor).expect(ERROR_TOKIO);
        wit_bindgen::block_on(server::server_main(request, response_out))
            .expect("error handling request");
    }
}
