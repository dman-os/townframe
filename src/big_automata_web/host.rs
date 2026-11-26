//! The page: sync one part from a native peer over iroh and report it to the DOM, or serve one
//! (`?mode=serve`).
//!
//! `fixture.json` (written by `x/big-automata-web.ts` from the `web_fixture` example's first
//! line of output) names the peer's relay and node id, the part to want, and how many objects
//! it holds. The browser has no UDP, so the endpoint is relay-only: it dials the peer by its
//! node id through the relay, over a WebSocket. The driver runs on the page's event loop
//! (`spawn_local`: the loop holds JS-backed futures and is not `Send`).
//!
//! The page is read through three elements: `#status` is `connecting`, then `Synced` once the
//! driver reports the part synced; `#objects` is then the part's member count in the local
//! store and `#expected` the count the peer published.

use std::sync::Arc;

use big_automata::PartKey;
use big_automata::PeerKey;
use big_automata::driver::DriverConfig;
use big_automata::driver::Facts;
use big_automata::driver::PeerDriver;
use big_automata::driver::RouteUpdate;
use big_automata::driver::memory::MemoryBackend;
use big_automata::driver::memory::MemoryPeerCursors;
use big_automata::part_store::PartStoreRead;
use big_automata::part_store::laws::add;
use big_automata::part_store::laws::grant;
use big_automata::part_store::laws::obj;
use big_automata::part_store::laws::principal;
use big_automata::part_store::laws::set;
use big_automata::part_store::laws::stage;
use big_automata::part_store::memory::MemoryPartStore;
use big_automata::part_store::oracle::PartWrite;
use big_automata::replay::wire::ReplayProtocol;
use big_automata::sync::rpc::SyncRpcProtocol;
use big_automata_iroh::PeerRpcClient;
use big_automata_iroh::serve::ReplayBinding;
use big_automata_iroh::serve::SyncRpcBinding;
use big_automata_iroh::serve::boot;
use method_core::ContextError;
use serde::Deserialize;
use service_client_iroh_rs::IROH_XRPC_HTTP_ALPN;
use service_client_iroh_rs::IrohHttp;
use service_utils_rs::RequestContext;
use service_utils_rs::ServerBinding;
use service_utils_rs::TransportContext;
use service_utils_rs::xrpc::XrpcRouter;
use service_utils_rs::xrpc::iroh_protocol_handler;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

/// What the peer published, as `web_fixture` prints it.
#[derive(Deserialize)]
struct Fixture {
    relay_url: iroh::RelayUrl,
    node_id: iroh::EndpointId,
    part: PartKey,
    objects: u64,
}

fn show(id: &str, text: &str) {
    web_sys::window()
        .expect("the host runs in a window")
        .document()
        .expect("a window has a document")
        .get_element_by_id(id)
        .unwrap_or_else(|| panic!("the page has no #{id}"))
        .set_text_content(Some(text));
}

async fn fetch_fixture() -> Fixture {
    let window = web_sys::window().expect("the host runs in a window");
    let response = JsFuture::from(window.fetch_with_str("fixture.json"))
        .await
        .expect("fetching fixture.json")
        .dyn_into::<web_sys::Response>()
        .expect("fetch resolves to a Response");
    assert!(response.ok(), "fixture.json: HTTP {}", response.status());
    let text = JsFuture::from(response.text().expect("a body"))
        .await
        .expect("reading fixture.json")
        .as_string()
        .expect("text() resolves to a string");
    serde_json::from_str(&text).expect("fixture.json is the fixture's output")
}

async fn run() {
    let fixture = fetch_fixture().await;

    // Relay-only: the page's only route to the peer is the relay of the fixture.
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::custom([fixture.relay_url.clone()]))
        .bind()
        .await
        .expect("binding the browser endpoint");
    let peer_addr = iroh::EndpointAddr::new(fixture.node_id).with_relay_url(fixture.relay_url);
    let rpc = PeerRpcClient::over_iroh(IrohHttp::new(endpoint.clone(), peer_addr, 1 << 20));

    let local = MemoryPartStore::default();
    let peer = PeerKey::new(fixture.node_id.as_bytes());
    let facts = Facts::new();
    // The page's own membership source (its keyhive stream) makes the published objects
    // members: a peer's jobs only notify, they never change membership.
    let backend = MemoryBackend::hinted(local.clone());
    for at in 1..=u8::try_from(fixture.objects).expect("the fixture publishes few objects") {
        backend
            .member_added(&obj(at), &fixture.part)
            .await
            .expect("the memory store cannot fail");
    }
    let (driver, driver_loop) = PeerDriver::start(
        DriverConfig::new(peer.clone(), "memory"),
        Arc::new(web_time::Instant::now),
        facts.clone(),
        backend,
        MemoryPeerCursors::default(),
        local.clone(),
    );
    wasm_bindgen_futures::spawn_local(driver_loop);
    driver
        .update_routes(RouteUpdate {
            added_parts: vec![fixture.part.clone()],
            ..Default::default()
        })
        .await;
    driver.connect(rpc);
    facts
        .wait_for_full_sync([(peer, fixture.part.clone())])
        .await
        .expect("the part stays wanted");

    let members = local
        .member_count(&fixture.part)
        .await
        .expect("the memory store cannot fail");
    show("objects", &members.to_string());
    show("expected", &fixture.objects.to_string());
    show("status", "Synced");
    // The driver and endpoint outlive this function: the page keeps following the peer.
    std::mem::forget(driver);
    std::mem::forget(endpoint);
}

/// Objects the serving page publishes in part 1, in as many commits so a puller sees more than
/// one rev. The fixture's own part has a different count, so a page that pulled instead of
/// serving would show.
const SERVED_OBJECTS: u8 = 5;

async fn commit(store: &MemoryPartStore, writes: Vec<PartWrite>) {
    let mut tx = store.begin();
    for write in writes {
        stage(store, &mut tx, write)
            .await
            .expect("the memory store cannot fail");
    }
    tx.commit().expect("the writes change a row");
}

/// `?mode=serve`: the page hosts the real big_automata service (the replay and sync rpc
/// protocols over a part store) behind the iroh protocol handler, and a native peer pulls
/// from it through the relay. `#node_id` carries the endpoint id once its home relay is
/// connected, `#served` the object count of the published part.
async fn run_serve() {
    let fixture = fetch_fixture().await;
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::custom([fixture.relay_url]))
        .bind()
        .await
        .expect("binding the browser endpoint");

    let store = MemoryPartStore::default();
    commit(&store, vec![grant(&[(1, 0)])]).await;
    for obj in 1..=SERVED_OBJECTS {
        commit(&store, vec![set(&[(obj, obj * 10)]), add(&[(obj, 1)])]).await;
    }
    // Every dialer acts as the principal the part is granted to: the page has no one to tell
    // peers apart, as the native fixture.
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
    let iroh_router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(
            IROH_XRPC_HTTP_ALPN,
            iroh_protocol_handler(router.into_router(), 4, 16),
        )
        .spawn();
    // The home relay is connected: a dialer that knows only our id and this relay reaches us.
    endpoint.online().await;
    show("served", &SERVED_OBJECTS.to_string());
    show("node_id", &endpoint.id().to_string());
    // The service outlives this function: the page keeps serving.
    std::mem::forget((iroh_router, replay, stop, endpoint));
}

/// The echo protocol of the accept probe: the dialer opens one bi stream, sends `ECHO_REQUEST`,
/// finishes its side, and the acceptor writes the same bytes back.
const ECHO_ALPN: &[u8] = b"townframe/echo-probe/0";
const ECHO_REQUEST: &[u8] = b"hello through the relay";

/// `?mode=accept`: the page accepts echo connections and publishes its endpoint id in `#node_id`.
async fn run_accept() {
    let fixture = fetch_fixture().await;
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::custom([fixture.relay_url]))
        .alpns(vec![ECHO_ALPN.to_vec()])
        .bind()
        .await
        .expect("binding the browser endpoint");
    // The home relay is connected: a dialer that knows only our id and this relay reaches us.
    endpoint.online().await;
    show("node_id", &endpoint.id().to_string());
    let counters = std::rc::Rc::new(std::cell::Cell::new((0u32, 0u32)));
    while let Some(incoming) = endpoint.accept().await {
        let counters = std::rc::Rc::clone(&counters);
        // One task per connection: the loop must keep accepting while a dialer lingers.
        wasm_bindgen_futures::spawn_local(async move {
            let connection = incoming
                .accept()
                .expect("accepting an incoming connection")
                .await
                .expect("completing the handshake");
            let (mut send, mut recv) = connection.accept_bi().await.expect("the dialer's stream");
            let request = recv
                .read_to_end(1 << 16)
                .await
                .expect("reading the request");
            send.write_all(&request).await.expect("echoing");
            send.finish().expect("finishing the echo");
            let (echoed, closed) = counters.get();
            counters.set((echoed + 1, closed));
            show("echoed", &(echoed + 1).to_string());
            // The dialer closes once it has the echo; observing that is the other half of the
            // connection's life cycle working.
            connection.closed().await;
            let (echoed, closed) = counters.get();
            counters.set((echoed, closed + 1));
            show("closed", &(closed + 1).to_string());
        });
    }
}

/// `?mode=dial&target=<id>`: dial another page's endpoint through the relay and check the echo.
async fn run_dial(target: iroh::EndpointId) {
    let fixture = fetch_fixture().await;
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::custom([fixture.relay_url.clone()]))
        .bind()
        .await
        .expect("binding the browser endpoint");
    let addr = iroh::EndpointAddr::new(target).with_relay_url(fixture.relay_url);
    let connection = endpoint
        .connect(addr, ECHO_ALPN)
        .await
        .expect("dialing the accepting page");
    let (mut send, mut recv) = connection.open_bi().await.expect("opening a stream");
    send.write_all(ECHO_REQUEST).await.expect("sending");
    send.finish().expect("finishing the request");
    let echo = recv.read_to_end(1 << 16).await.expect("reading the echo");
    assert_eq!(echo, ECHO_REQUEST, "the echo differs from the request");
    connection.close(0u8.into(), b"done");
    // `close` only queues the CONNECTION_CLOSE; dropping the endpoint at the end of this task
    // would cancel it before it is sent and the acceptor would never see the close.
    // `Endpoint::close` drains it first.
    endpoint.close().await;
    show("echo", &String::from_utf8(echo).expect("the echo is text"));
}

// Counts the allocations of `?mode=bench`; costs one thread-local read per allocation otherwise.
#[global_allocator]
static HEAP: utils_rs::testing::alloc_count::CountingAllocator =
    utils_rs::testing::alloc_count::CountingAllocator;

/// `?mode=bench&n=<objects>&rtt_ms=<ms>`: the whole-sync workload of `big_automata::bench` with
/// two in-memory nodes in this page, timed by `performance.now()` (what `web_time` reads) and
/// reported, never gated: a browser has no instruction counter. `#bench` holds one
/// `BENCH_RECORD` line per measurement.
async fn run_bench(objects: usize, rtt_ms: u64) {
    let clock: big_automata::Clock = Arc::new(web_time::Instant::now);
    let report = big_automata::bench::sync_once(
        objects,
        std::time::Duration::from_millis(rtt_ms),
        |_| {},
        clock,
    )
    .await;
    show(
        "bench",
        &report.record_lines("big_automata::whole_sync").join("\n"),
    );
}

pub fn main() {
    console_error_panic_hook::set_once();
    let search = web_sys::window()
        .expect("the host runs in a window")
        .location()
        .search()
        .expect("the location has a query");
    let param = |name: &str| {
        search
            .trim_start_matches('?')
            .split('&')
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
            .map(str::to_owned)
    };
    match param("mode").as_deref() {
        None => wasm_bindgen_futures::spawn_local(run()),
        Some("accept") => wasm_bindgen_futures::spawn_local(run_accept()),
        Some("serve") => wasm_bindgen_futures::spawn_local(run_serve()),
        Some("dial") => {
            let target = param("target").expect("dial mode takes target=<endpoint id>");
            wasm_bindgen_futures::spawn_local(run_dial(
                target.parse().expect("target is an endpoint id"),
            ));
        }
        Some("bench") => {
            let number = |name: &str, default: u64| {
                param(name).map_or(default, |value| value.parse().expect("a number"))
            };
            wasm_bindgen_futures::spawn_local(run_bench(
                usize::try_from(number("n", 1000)).expect("fits"),
                number("rtt_ms", 0),
            ));
        }
        Some(other) => panic!("unknown mode {other}"),
    }
}
