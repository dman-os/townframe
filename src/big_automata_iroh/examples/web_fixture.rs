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
//! Its stdin carries commands, one per line, and it runs until stdin closes, so a parent that dies
//! takes it along. `dial <endpoint id>` dials that endpoint (a browser page) over the relay with
//! the echo probe's ALPN, sends `ECHO_REQUEST`, and answers with one stdout line, `ok` when the
//! echo matches or `err <why>`: the probe of whether a browser endpoint can accept.
//! `pull <endpoint id>` runs a `PeerDriver` against that endpoint (a page serving big_automata
//! over iroh, `?mode=serve`) until part 1 is fully synced into a local store, and answers
//! `synced <objects>`, the part's member count there, or `err <why>`.

use std::net::Ipv4Addr;
use std::sync::Arc;

use big_automata::PeerKey;
use big_automata::driver::DriverConfig;
use big_automata::driver::Facts;
use big_automata::driver::PeerDriver;
use big_automata::driver::RouteUpdate;
use big_automata::driver::memory::LwwPayload;
use big_automata::driver::memory::MemoryBackend;
use big_automata::driver::memory::MemoryPeerCursors;
use big_automata::part_store::PartStoreRead;
use big_automata::part_store::laws::add;
use big_automata::part_store::laws::grant;
use big_automata::part_store::laws::obj;
use big_automata::part_store::laws::part;
use big_automata::part_store::laws::principal;
use big_automata::part_store::laws::stage;
use big_automata::part_store::memory::MemoryPartStore;
use big_automata::part_store::oracle::PartWrite;
use big_automata::replay::wire::ReplayProtocol;
use big_automata::sync::rpc::SyncRpcProtocol;
use big_automata_iroh::PeerRpcClient;
use big_automata_iroh::serve::ReplayBinding;
use big_automata_iroh::serve::SyncRpcBinding;
use big_automata_iroh::serve::boot;
use iroh_relay::server::RelayConfig;
use iroh_relay::server::Server;
use iroh_relay::server::ServerConfig;
use method_core::ContextError;
use service_client_iroh_rs::IROH_XRPC_HTTP_ALPN;
use service_client_iroh_rs::IrohHttp;
use service_utils_rs::RequestContext;
use service_utils_rs::ServerBinding;
use service_utils_rs::TransportContext;
use service_utils_rs::xrpc::XrpcRouter;
use service_utils_rs::xrpc::iroh_protocol_handler;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;

/// Mirrors the page's echo probe (`big_automata_web`'s host).
const ECHO_ALPN: &[u8] = b"townframe/echo-probe/0";
const ECHO_REQUEST: &[u8] = b"hello through the relay";

async fn dial_echo(
    node: &iroh::Endpoint,
    relay_url: &iroh::RelayUrl,
    target: iroh::EndpointId,
) -> Result<(), String> {
    let addr = iroh::EndpointAddr::new(target).with_relay_url(relay_url.clone());
    let connection = node
        .connect(addr, ECHO_ALPN)
        .await
        .map_err(|err| format!("connect: {err:?}"))?;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| format!("open_bi: {err:?}"))?;
    send.write_all(ECHO_REQUEST)
        .await
        .map_err(|err| format!("write: {err:?}"))?;
    send.finish().map_err(|err| format!("finish: {err:?}"))?;
    let echo = recv
        .read_to_end(1 << 16)
        .await
        .map_err(|err| format!("read: {err:?}"))?;
    connection.close(0u8.into(), b"done");
    if echo == ECHO_REQUEST {
        Ok(())
    } else {
        Err(format!("echo differs: {echo:?}"))
    }
}

/// Syncs part 1 from the page serving at `target` into a fresh local store, through the relay,
/// and returns how many objects the part holds there.
async fn pull_part(
    node: &iroh::Endpoint,
    relay_url: &iroh::RelayUrl,
    target: iroh::EndpointId,
) -> Result<u64, String> {
    let addr = iroh::EndpointAddr::new(target).with_relay_url(relay_url.clone());
    let rpc = PeerRpcClient::over_iroh(IrohHttp::new(node.clone(), addr, 1 << 20));
    let local = MemoryPartStore::default();
    let peer = PeerKey::new(target.as_bytes());
    let facts = Facts::new();
    // The page's own membership source (its keyhive stream) makes the published objects
    // members: a peer's jobs only notify, they never change membership.
    let backend = MemoryBackend::hinted(local.clone());
    for at in 1..=OBJECTS {
        backend
            .member_added(&obj(at), &part(1))
            .await
            .map_err(|err| format!("member_added: {err:?}"))?;
    }
    let (driver, driver_loop) = PeerDriver::start(
        DriverConfig::new(peer.clone(), "memory"),
        Arc::new(std::time::Instant::now),
        facts.clone(),
        backend,
        MemoryPeerCursors::default(),
        local.clone(),
    );
    let driver_task = tokio::spawn(driver_loop);
    driver
        .update_routes(RouteUpdate {
            added_parts: vec![part(1)],
            ..Default::default()
        })
        .await;
    driver.connect(rpc);
    let synced = facts.wait_for_full_sync([(peer, part(1))]).await;
    drop(driver);
    driver_task.abort();
    synced.map_err(|err| format!("sync: {err:?}"))?;
    local
        .member_count(&part(1))
        .await
        .map_err(|err| format!("member_count: {err:?}"))
}

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
        commit(
            &store,
            vec![LwwPayload::set(&[(obj, obj * 10)]), add(&part_member)],
        )
        .await;
    }

    let principal_of = |_: &RequestContext,
                        _: &TransportContext|
     -> Result<PeerKey, ContextError> { Ok(principal(0)) };
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

    let mut commands = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = commands.next_line().await.unwrap() {
        let (command, target) = line
            .split_once(' ')
            .unwrap_or_else(|| panic!("unknown fixture command {line:?}"));
        let target: iroh::EndpointId = target.trim().parse().expect("commands take an endpoint id");
        match command {
            "dial" => match dial_echo(&node, &relay_url, target).await {
                Ok(()) => println!("ok"),
                Err(why) => println!("err {why}"),
            },
            "pull" => match pull_part(&node, &relay_url, target).await {
                Ok(objects) => println!("synced {objects}"),
                Err(why) => println!("err {why}"),
            },
            other => panic!("unknown fixture command {other:?}"),
        }
    }

    iroh_router.shutdown().await.expect("router shutdown");
    node.close().await;
    stop.stop().await;
    relay.shutdown().await.unwrap();
}
