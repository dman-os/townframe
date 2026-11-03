//! Spike of the inbox call model, see `wit/inbox.wit`.
//!
//! On `wasm32` this crate is the guest fixture: `serve` reads requests from its inbox and runs
//! one handler per request concurrently inside the serve task, with no spawned tasks (a
//! `FuturesUnordered` the serve task polls itself), so the export's task state is entered once.
//! Off `wasm32` it holds only the host tests that run that fixture on Wasmtime and on the WCMP
//! polyfill.
//!
//! The handlers, by `request.publication`:
//! * `echo` answers with the payload;
//! * `gate` awaits the host import `gate(payload as a name)` and answers with what it returns;
//! * `drop` drops its responder unanswered.

#[cfg(target_arch = "wasm32")]
mod guest;

#[cfg(all(test, not(target_arch = "wasm32")))]
#[macro_use]
mod host;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod wasmtime_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod wcmp_tests;
