//! The page: sync one part from a native peer over iroh and report it to the DOM.
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
use big_automata::part_store::memory::MemoryPartStore;
use big_automata_iroh::PeerRpcClient;
use serde::Deserialize;
use service_client_iroh_rs::IrohHttp;
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
    let (driver, driver_loop) = PeerDriver::start(
        DriverConfig::new(peer.clone(), "memory"),
        Arc::new(web_time::Instant::now),
        facts.clone(),
        MemoryBackend::hinted(local.clone()),
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
        let counters = counters.clone();
        // One task per connection: the loop must keep accepting while a dialer lingers.
        wasm_bindgen_futures::spawn_local(async move {
            let connection = incoming
                .accept()
                .expect("accepting an incoming connection")
                .await
                .expect("completing the handshake");
            let (mut send, mut recv) =
                connection.accept_bi().await.expect("the dialer's stream");
            let request = recv.read_to_end(1 << 16).await.expect("reading the request");
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
        Some("dial") => {
            let target = param("target").expect("dial mode takes target=<endpoint id>");
            wasm_bindgen_futures::spawn_local(run_dial(
                target.parse().expect("target is an endpoint id"),
            ));
        }
        Some(other) => panic!("unknown mode {other}"),
    }
}
