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

pub fn main() {
    console_error_panic_hook::set_once();
    wasm_bindgen_futures::spawn_local(run());
}
