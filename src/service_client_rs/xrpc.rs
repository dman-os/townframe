mod client;
#[cfg(feature = "iroh-http")]
pub mod iroh;
pub mod ws;

pub use client::XrpcClient;
pub use client::XrpcClientError;
#[cfg(feature = "iroh-http")]
pub use iroh::IROH_XRPC_HTTP_ALPN;
#[cfg(feature = "iroh-http")]
pub use iroh::IrohXrpcClient;
