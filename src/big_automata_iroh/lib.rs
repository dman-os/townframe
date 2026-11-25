//! The big_automata sync protocols bound onto the service stack, in two halves.
//!
//! The calling half, [`PeerRpcClient`], is `BigSyncRpcClient` over typed clients of a host that
//! serves [`ReplayProtocol`](big_automata::replay::wire::ReplayProtocol) and its sibling
//! [`SyncRpcProtocol`](big_automata::sync::rpc::SyncRpcProtocol). It builds for the browser
//! (`wasm32-unknown-unknown`) on `service_client_iroh_rs`'s `IrohHttp`.
//!
//! The serving half, [`serve`], hosts those protocols from a part store: `ReplayService`
//! answers the six methods and `boot` hosts them the way `big_blobs` hosts its service. It
//! supplies what the sans-IO half cannot: a lock around the session table and the embedder's mapping from a request to the principal whose access the
//! store decides by. It sits on `service_utils_rs`'s portable core (no TCP or Unix serving), so
//! a browser node serves the protocols to its peers over iroh too.
//!
//! The protocols' laws live in `big_automata::replay` and `big_automata::sync::rpc`; this crate
//! adds no protocol semantics.

mod client;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod e2e;
pub mod serve;

pub use client::PeerRpcClient;
