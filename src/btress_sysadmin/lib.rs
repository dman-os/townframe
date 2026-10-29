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
        path: "wit",
        generate_all,
        async: ["export:wasi:http/handler@0.3.0#handle"],
    });
}

#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    use crate::app::*;
    console_error_panic_hook::set_once();
    leptos::mount::hydrate_body(App);
}

#[cfg(all(feature = "ssr", target_os = "wasi"))]
#[unsafe(no_mangle)]
unsafe extern "Rust" fn __getrandom_v03_custom(
    dest: *mut u8,
    len: usize,
) -> Result<(), getrandom_03::Error> {
    let destination = unsafe { core::slice::from_raw_parts_mut(dest, len) };
    let mut offset = 0;
    while offset < len {
        let random = wit::wasi::random::random::get_random_bytes((len - offset) as u64);
        if random.is_empty() {
            return Err(getrandom_03::Error::UNEXPECTED);
        }
        let amount = random.len().min(len - offset);
        destination[offset..offset + amount].copy_from_slice(&random[..amount]);
        offset += amount;
    }
    Ok(())
}
#[cfg(feature = "ssr")]
use crate::interlude::*;
#[cfg(feature = "ssr")]
use wit::wasi::http::types::Request;
#[cfg(feature = "ssr")]
use wit::wasi::http::types::Response;

#[cfg(feature = "ssr")]
struct Component;

#[cfg(feature = "ssr")]
wit::export!(Component with_types_in wit);

#[cfg(feature = "ssr")]
impl wit::exports::wasi::http::handler::Guest for Component {
    async fn handle(request: Request) -> Result<Response, wit::wasi::http::types::ErrorCode> {
        any_spawner::Executor::init_tokio().expect(ERROR_TOKIO);
        Ok(server::server_main(request)
            .await
            .expect("error handling request"))
    }
}
