//! Entry point for the browser composition shell (ADR 018 §7).
//!
//! The shell only has behaviour in a browser: it fetches the counter guest and compiles it on
//! the page's own `WebAssembly` engine. Cargo nonetheless builds every workspace binary for
//! the native test and coverage lanes, where there is no window, no guest fetch, and no
//! renderer, so the native binary is deliberately empty instead of pretending to run.

#[cfg(target_arch = "wasm32")]
mod shell;

#[cfg(target_arch = "wasm32")]
fn main() {
    shell::main();
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
