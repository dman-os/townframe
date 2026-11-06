//! The typed service client for every build target.
//!
//! [`Client`] is the call API over [`service_core_rs::Operation`] types. What carries a call to
//! its server is a transport, a type implementing [`UnaryTransport`] and
//! [`SubscriptionTransport`]. This crate holds no I/O and builds for native hosts and wasm guests
//! alike; the native transports (in-process, irpc, hyper XRPC) live in
//! `service_client_native_rs`, and a guest implements [`xrpc::HttpTransport`] over its own HTTP
//! import, which only moves bytes because [`xrpc`] owns the envelope encoding and decoding.

pub mod client;
pub mod xrpc;

pub use client::Client;
pub use client::ClientStream;
pub use client::Subscribed;
pub use client::SubscriptionTransport;
pub use client::UnaryTransport;
