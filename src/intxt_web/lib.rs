//! A read-only, server-rendered viewer of intxts, as a `wasi:http/handler` component.
//!
//! The page is `view(state)`: [`source`] loads an [`view::IntxtView`] on the server, [`ui`] renders
//! it, [`route`] connects a request path to the two. [`source`] has the fixture, [`node`] reads a
//! day node over its XRPC. There are no mutations, accounts, sessions,
//! server functions or hydration.

pub mod diff;
pub mod feed;
pub mod files;
pub mod guide;
pub mod markdown;
pub mod node;
pub mod people;
pub mod route;
pub mod session;
pub mod source;
pub mod ui;
pub mod view;

// The wasi bindings only exist for the component; the rest builds and tests on the host.
#[cfg(target_arch = "wasm32")]
mod wasi_http;
