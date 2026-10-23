pub use service_core_rs::xrpc::*;

mod client;
#[cfg(feature = "iroh-http")]
pub mod iroh;
pub mod ws;

pub use client::{XrpcClient, XrpcClientError};
#[cfg(feature = "iroh-http")]
pub use iroh::{IROH_XRPC_HTTP_ALPN, IrohXrpcClient};
