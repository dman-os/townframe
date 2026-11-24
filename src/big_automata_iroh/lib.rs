//! The big_automata sync protocols bound onto the service stack, in two halves.
//!
//! The calling half, [`PeerRpcClient`], is `BigSyncRpcClient` over typed clients of a host that
//! serves [`ReplayProtocol`](big_automata::replay::wire::ReplayProtocol) and its sibling
//! [`SyncRpcProtocol`](big_automata::sync::rpc::SyncRpcProtocol). It builds for the browser
//! (`wasm32-unknown-unknown`) on `service_client_iroh_rs`'s `IrohHttp`, and is always present.
//!
//! The serving half, behind the default `serve` feature, hosts those protocols from a part
//! store: [`ReplayService`] answers the six methods and [`boot`] hosts them the way `big_blobs`
//! hosts its service. It supplies what the sans-IO half cannot: a lock around the session
//! table, a tokio timer for held rounds, and the embedder's mapping from a request to the
//! principal whose access the store decides by. A browser build turns `serve` off: the host
//! stack is a native serving host and does not build there.
//!
//! The protocols' laws live in `big_automata::replay` and `big_automata::sync::rpc`; this crate
//! adds no protocol semantics.

mod client;
#[cfg(feature = "serve")]
mod serve;
#[cfg(all(test, feature = "serve"))]
mod tests;

pub use client::PeerRpcClient;
#[cfg(feature = "serve")]
pub use serve::Replay;
#[cfg(feature = "serve")]
pub use serve::ReplayBinding;
#[cfg(feature = "serve")]
pub use serve::ReplayService;
#[cfg(feature = "serve")]
pub use serve::SyncRpcBinding;
#[cfg(feature = "serve")]
pub use serve::boot;
