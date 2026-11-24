//! The big_automata sync protocols bound onto the service stack, in two halves.
//!
//! The calling half, [`PeerRpcClient`], is `BigSyncRpcClient` over typed clients of a host that
//! serves [`ReplayProtocol`](big_automata::replay::wire::ReplayProtocol) and its sibling
//! [`SyncRpcProtocol`](big_automata::sync::rpc::SyncRpcProtocol). It builds for the browser
//! (`wasm32-unknown-unknown`) on `service_client_iroh_rs`'s `IrohHttp`.
//!
//! The serving half, [`serve`], hosts those protocols from a part store: `ReplayService`
//! answers the six methods and `boot` hosts them the way `big_blobs` hosts its service. It
//! supplies what the sans-IO half cannot: a lock around the session table, a tokio timer for
//! held rounds, and the embedder's mapping from a request to the principal whose access the
//! store decides by. It sits on the native host stack (tokio `net`), which has no browser build,
//! so the module and its dependencies exist on every target except the wasm32 ones.
//!
//! The protocols' laws live in `big_automata::replay` and `big_automata::sync::rpc`; this crate
//! adds no protocol semantics.

mod client;
#[cfg(not(target_arch = "wasm32"))]
pub mod serve;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

pub use client::PeerRpcClient;
