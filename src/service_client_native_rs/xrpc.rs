#[cfg(feature = "xrpc")]
mod client;
pub mod ws;

#[cfg(feature = "xrpc")]
pub use client::XrpcClient;
