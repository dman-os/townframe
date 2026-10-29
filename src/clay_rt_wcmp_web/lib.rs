//! The browser backend for `clay_rt_wcmp`: components compile on the page's own
//! `WebAssembly` engine. Everything else — the generated bindings, the WASI host, the
//! resource tables, and store semantics — is shared with the native and Wasmi backends, so
//! the three cannot drift apart.
//!
//! The shell supplies bytes; this module supplies the engine and the linker.
//!
//! The backend it wraps, `wcmp-wasm-core-web`, is the JavaScript API and exists only on
//! `wasm32`. So is this crate: off `wasm32` it compiles empty, which is what lets the shared
//! composition crates depend on it unconditionally and still build for the native lanes.

#![cfg(target_arch = "wasm32")]

/// Compile `bytes` for the browser and assemble the shared linker over them.
///
/// # Errors
///
/// Compilation or linker assembly failed; the message is for the shell to report.
pub async fn load(bytes: &[u8]) -> Result<clay_rt_wcmp::WcmpPackage, String> {
    let backend = wcmp_wasm_core_web::Web::new();
    let engine = wcmp::Engine::with_backend(backend).map_err(|error| error.to_string())?;
    clay_rt_wcmp::WcmpPackage::load(engine, bytes).await
}
