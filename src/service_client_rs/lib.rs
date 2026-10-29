pub mod client;
#[cfg(feature = "http")]
pub mod xrpc;

pub use client::Client;
pub use client::ClientStream;
pub use client::InlineRoute;
#[cfg(feature = "http")]
pub use xrpc::XrpcClient;
#[cfg(feature = "http")]
pub use xrpc::XrpcClientError;
