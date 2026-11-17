//! The native transports of the typed service client: in-process routing to a host, irpc channels,
//! and XRPC over HTTP (TCP or a Unix socket, plus iroh with the `iroh-http` feature).
//!
//! [`Transport<Proto>`] is the one concrete transport service code stores, whatever carries its
//! calls, and [`Client<Proto>`] is the client over it:
//! `Client::new(Transport::direct(host))`. The client core and the XRPC envelope codec live in
//! `service_client_core_rs`, which also builds for wasm guests.

mod transport;
pub mod xrpc;

pub use transport::Client;
pub use transport::InlineRoute;
pub use transport::Transport;
pub use xrpc::XrpcClient;
