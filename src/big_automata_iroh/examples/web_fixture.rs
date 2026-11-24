//! The native peer of the browser run (`src/big_automata_web`): a relay, and a node serving a
//! part store over iroh through that relay.
//!
//! It starts an `iroh-relay` server in plain-HTTP dev mode (no TLS, no QUIC address discovery:
//! a browser reaches a relay over a WebSocket, and a page served over `http://` may open `ws://`
//! to loopback), a `MemoryPartStore` with one published part, and `boot` over an iroh endpoint
//! whose only relay is that one. Every endpoint that connects acts as one principal, the one the
//! part is granted to: the fixture has no one to tell clients apart, so the browser's random
//! endpoint id is admitted as it is.
//!
//! One JSON line on stdout says what a page needs to connect and what it should end up with:
//!
//! ```json
//! {"relay_url":"http://127.0.0.1:41233/","node_id":"…","part":"…","objects":7}
//! ```
//!
//! The fixture runs until its stdin closes, so a parent that dies takes it along.

use std::net::Ipv4Addr;
use std::sync::Arc;

use big_automata::PeerKey;
use big_automata::part_store::laws::add;
use big_automata::part_store::laws::grant;
use big_automata::part_store::laws::part;
use big_automata::part_store::laws::principal;
use big_automata::part_store::laws::set;
use big_automata::part_store::laws::stage;
use big_automata::part_store::memory::MemoryPartStore;
use big_automata::part_store::oracle::PartWrite;
use big_automata::replay::wire::ReplayProtocol;
use big_automata::sync::rpc::SyncRpcProtocol;
use big_automata_iroh::serve::ReplayBinding;
use big_automata_iroh::serve::SyncRpcBinding;
use big_automata_iroh::serve::boot;
use iroh_relay::server::RelayConfig;
use iroh_relay::server::Server;
use iroh_relay::server::ServerConfig;
use method_core::ContextError;
use service_client_iroh_rs::IROH_XRPC_HTTP_ALPN;
use service_utils_rs::RequestContext;
use service_utils_rs::ServerBinding;
use service_utils_rs::TransportContext;
use service_utils_rs::xrpc::XrpcRouter;
use service_utils_rs::xrpc::iroh_protocol_handler;
use tokio::io::AsyncReadExt;

/// Objects published in the part, in two commits so the page pulls more than one rev.
const OBJECTS: u8 = 7;

async fn commit(store: &MemoryPartStore, writes: Vec<PartWrite>) {
    let mut tx = store.begin();
    for write in writes {
        stage(store, &mut tx, write).await.unwrap();
    }
    tx.commit().expect("the writes change a row");
}

#[tokio::main]
async fn main() {
    let mut relay_config = ServerConfig::default();
    relay_config.relay = Some(RelayConfig::new((Ipv4Addr::LOCALHOST, 0)));
    let relay = Server::spawn(relay_config).await.unwrap();
    let relay_url: iroh::RelayUrl = format!("http://{}", relay.http_addr().unwrap())
        .parse()
        .unwrap();

    let node = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::custom([relay_url.clone()]))
        .bind()
        .await
        .unwrap();
    // The home relay is connected: `addr()` below carries it, which is all a browser dials by.
    node.online().await;

    let store = MemoryPartStore::default();
    commit(&store, vec![grant(&[(1, 0)])]).await;
    for obj in 1..=OBJECTS {
        let part_member = [(obj, 1)];
        commit(&store, vec![set(&[(obj, obj * 10)]), add(&part_member)]).await;
    }

    let principal_of = |_: &RequestContext, _: &TransportContext| -> Result<PeerKey, ContextError> {
        Ok(principal(0))
    };
    let (replay, stop) = boot(store, principal_of);
    let mut router = XrpcRouter::new(1 << 20);
    <ReplayBinding<_> as ServerBinding<ReplayProtocol>>::mount(
        &mut router,
        ReplayBinding::new(Arc::clone(&replay.host)),
    )
    .expect("mount the replay protocol");
    <SyncRpcBinding<_> as ServerBinding<SyncRpcProtocol>>::mount(
        &mut router,
        SyncRpcBinding::new(Arc::clone(&replay.host)),
    )
    .expect("mount the sync rpc protocol");
    let iroh_router = iroh::protocol::Router::builder(node.clone())
        .accept(
            IROH_XRPC_HTTP_ALPN,
            iroh_protocol_handler(router.into_router(), 4, 16),
        )
        .spawn();

    println!(
        "{}",
        serde_json::json!({
            "relay_url": relay_url,
            "node_id": node.id().to_string(),
            "part": part(1).to_string(),
            "objects": OBJECTS,
        })
    );

    let mut sink = Vec::new();
    tokio::io::stdin().read_to_end(&mut sink).await.unwrap();

    iroh_router.shutdown().await.expect("router shutdown");
    node.close().await;
    stop.stop().await;
    relay.shutdown().await.unwrap();
}
