//! Entry point of the browser host (see `host`). Cargo builds every workspace binary for the
//! native lanes too, where there is no page to report to, so the native binary is empty.

#[cfg(target_arch = "wasm32")]
mod host;

#[cfg(target_arch = "wasm32")]
fn main() {
    host::main();
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
