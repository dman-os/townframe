//! The native transports of the typed service client: in-process routing to a host, irpc channels,
//! XRPC over HTTP (TCP or a Unix socket), and with the `iroh` feature XRPC over iroh streams
//! (`service_client_iroh_rs`, which also builds for the browser).
//!
//! [`Transport<Proto>`] is the one concrete transport service code stores, whatever carries its
//! calls, and [`Client<Proto>`] is the client over it:
//! `Client::new(Transport::direct(host, origin))`. The client core and the XRPC envelope codec live in
//! `service_client_core_rs`, which also builds for wasm guests.
//!
//! The crate builds for `wasm32-unknown-unknown` without the `xrpc` feature, which adds the
//! transport that needs reqwest and tokio's sockets (`XrpcClient` and `Transport::Xrpc`).

mod transport;
pub mod xrpc;

pub use transport::Client;
pub use transport::InlineRoute;
pub use transport::Transport;
#[cfg(feature = "xrpc")]
pub use xrpc::XrpcClient;
